import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import numpy as np

def tsne_viz(mat, rownames, use_pca=True, learning_rate=1000, n_iter=1000,colors=None, output_filename=None, figheight=40, figwidth=50):     
    """2d plot of `mat` using t-SNE, with the points labeled by `rownames`, 
        aligned with `colors` (defaults to all black).
        
        Parameters
        ----------    
        mat : 2d np.array
            The matrix to visualize.
        rownames : list of str
            Names of the points to visualize.
        use_pca : A Boolean flag,
            If True PCA will be used to reduce dimensions to 50.
        learning_rate : integer value
            Default is 1000, varies between 100 to 1000.
        n_iter : integer
            Maximum number of iteration for optimizations. 
        colors : list of colornames or None (default: None)
            Optional list of colors for rownames. The color names just need to 
            be interpretable by matplotlib. If they are supplied, they need to 
            have the same length as rownames, or indices if that is not None. 
            If `colors=None`, then all the words are displayed in black.
        output_filename : str (default: None)
            If not None, then the output image is written to this location. The 
            filename suffix determines the image type. If None, then 
            `plt.plot()` is called, with the behavior determined by the 
            environment.
        figheight : int (default: 40)
            Height in display units of the output.
        figwidth : int (default: 50)
            Width in display units of the output.
    """
    indices = list(range(len(rownames)))
    # Colors:
    if not colors:
        colors = ['black' for i in indices]    
    # Recommended reduction via PCA or similar:
    if use_pca:
        n_components = 50 if mat.shape[1] >= 50 else mat.shape[1]
        dimreduce = PCA(n_components=n_components)
        mat = dimreduce.fit_transform(mat)
    # t-SNE:
    tsne = TSNE(n_components=2, learning_rate=learning_rate, random_state=0, n_iter=n_iter)
    np.set_printoptions(suppress=True)    
    tsnemat = tsne.fit_transform(mat) 
    # Plot values:
    vocab = np.array(rownames)[indices]
    xvals = tsnemat[indices, 0] 
    yvals = tsnemat[indices, 1]
    # Plotting:
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_figheight(40)
    fig.set_figwidth(50)
    ax.plot(xvals, yvals, marker='', linestyle='')
    # Text labels:
    for word, x, y, color in zip(vocab, xvals, yvals, colors):
        ax.annotate(word, (x, y), fontsize=8, color=color)
    # Output:
    if output_filename:
        plt.savefig(output_filename, bbox_inches='tight')
    else:
        plt.show()
